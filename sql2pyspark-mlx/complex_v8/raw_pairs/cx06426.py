from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_end_date_month_product = Window.partitionBy(F.month("end_date"), "product")
w_end_date_year_product_by_pre = Window.partitionBy(F.year("end_date"), "product").orderBy(F.col("premium").asc_nulls_last(), "id")
w_end_date_year_product_by_pre_2 = Window.partitionBy(F.year("end_date"), "product").orderBy(F.col("premium").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_policies
    .filter((~F.col("status").isin("CANCELLED", "EXPIRED")) & (F.col("premium") < 3817.5))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("end_date")).alias("days_since_end_date"),
    )
)

metrics = (
    prepared
    .select(
        "*",
        F.size(F.collect_set("holder_id").over(w_end_date_month_product)).alias("distinct_holder_id_in_end_date_month_product"),
        (F.col("premium") - F.lag("premium").over(w_end_date_year_product_by_pre)).alias("premium_change"),
        F.round(F.col("premium") - F.avg("premium").over(w_end_date_month_product), 2).alias("premium_vs_end_date_month_product_avg"),
        F.sum("days_since_end_date").over(w_end_date_year_product_by_pre_2).alias("running_days_since_end_date"),
    )
)

result = (
    metrics
    .groupingSets(
        [["distinct_holder_id_in_end_date_month_product", "product", "holder_id"], ["distinct_holder_id_in_end_date_month_product"], ["product"]],
        "distinct_holder_id_in_end_date_month_product", "product", "holder_id",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.round(F.stddev("running_days_since_end_date"), 2).alias("stddev_running_days_since_end_date"),
        F.count("*").alias("row_count"),
    )
    .select(
        "distinct_holder_id_in_end_date_month_product",
        F.coalesce(F.col("product"), F.lit("ALL")).alias("product"),
        "holder_id",
        "grouping_level",
        "stddev_running_days_since_end_date",
        "row_count",
    )
    .orderBy(
        F.col("distinct_holder_id_in_end_date_month_product").asc_nulls_last(),
        F.col("product").asc_nulls_last(),
        F.col("holder_id").asc_nulls_last(),
    )
)
