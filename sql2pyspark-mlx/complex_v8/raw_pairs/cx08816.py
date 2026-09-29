from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_by_n_rows = Window.partitionBy("product").orderBy("n_rows", "product")
w_all_by_stddev_premium_full = Window.orderBy(F.col("stddev_premium").desc(), "product").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped_policys = (
    df_policies
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
    )
)

rollup_base = (
    scoped_policys
    .groupBy("product")
    .agg(
        F.round(F.stddev("premium"), 2).alias("stddev_premium"),
        F.count("*").alias("n_rows"),
    )
)

with_trends = (
    rollup_base
    .select(
        "product",
        "n_rows",
        F.round(F.percent_rank().over(w_product_by_n_rows), 3).alias("n_rows_percent_rank"),
        F.nth_value("n_rows", 2).over(w_all_by_stddev_premium_full).alias("second_n_rows"),
    )
)

with_activity = (
    with_trends
    .crossJoin(with_trends.agg(F.avg("n_rows").alias("overall_avg_n_rows")))
    .filter(F.col("n_rows") > F.col("overall_avg_n_rows"))
    .drop("overall_avg_n_rows")
)

result = (
    with_activity
    .select("product", "second_n_rows", "n_rows")
    .unpivot(
        ids=["product"],
        values=["second_n_rows", "n_rows"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
