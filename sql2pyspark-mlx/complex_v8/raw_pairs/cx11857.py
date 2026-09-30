from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_premium_ties = Window.orderBy(F.col("premium").asc_nulls_last())
w_product_holder_id_by_end_dat = Window.partitionBy("product", "holder_id").orderBy(F.col("end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_first_start_date_year_by_pre = Window.partitionBy(F.year("first_start_date")).orderBy(F.col("premium_cume_dist").desc(), "id")
w_all_by_end_date = Window.orderBy("end_date", "id")
w_product_holder_id = Window.partitionBy("product", "holder_id")
w_all_by_end_date_moving_2 = Window.orderBy("end_date", "id").rowsBetween(-2, Window.currentRow)

prepared = (
    df_policies
    .filter(F.col("premium").between(1541.8, 4688.7))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        F.add_months(F.col("end_date"), 3).alias("end_date_plus_3m"),
        F.date_format(F.col("end_date"), "yyyy-MM").alias("end_date_label"),
        F.date_trunc("quarter", F.col("end_date")).alias("end_date_quarter"),
    )
)

with_windows = (
    prepared
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "end_date_plus_3m",
        "end_date_label",
        "end_date_quarter",
        F.round(F.cume_dist().over(w_all_by_premium_ties), 3).alias("premium_cume_dist"),
        F.first("start_date").over(w_product_holder_id_by_end_dat).alias("first_start_date"),
    )
)

product_stats = (
    with_windows
    .groupBy("product")
    .agg(F.avg("premium_cume_dist").alias("avg_premium_cume_dist"))
)

vs_group = (
    with_windows.alias("s")
    .join(product_stats.alias("g"), F.col("g.product") == F.col("s.product"), "inner")
    .filter(F.col("s.premium_cume_dist") > F.col("g.avg_premium_cume_dist"))
    .select(
        "s.*",
        "g.avg_premium_cume_dist",
        F.round(F.col("s.premium_cume_dist") - F.col("g.avg_premium_cume_dist"), 2).alias("premium_cume_dist_minus_avg"),
    )
)

sequenced = (
    vs_group
    .select(
        "*",
        F.ntile(4).over(w_first_start_date_year_by_pre).alias("premium_cume_dist_quartile"),
        F.datediff(F.col("end_date_plus_3m"), F.lag("end_date_plus_3m").over(w_all_by_end_date)).alias("days_since_prev_end_date_plus_3m"),
        F.round(F.col("premium_cume_dist_minus_avg") - F.avg("premium_cume_dist_minus_avg").over(w_product_holder_id), 2).alias("premium_cume_dist_minus_avg_vs_product_holder_id"),
        F.round(F.avg("avg_premium_cume_dist").over(w_all_by_end_date_moving_2), 2).alias("avg_premium_cume_dist_avg_3"),
    )
)

result = (
    sequenced
    .select("id", "avg_premium_cume_dist_avg_3", "avg_premium_cume_dist")
    .unpivot(
        ids=["id"],
        values=["avg_premium_cume_dist_avg_3", "avg_premium_cume_dist"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
