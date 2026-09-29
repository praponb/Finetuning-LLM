from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_end_date_month_by_en = Window.partitionBy("product", F.month("end_date")).orderBy("end_date", "id")
w_all_by_end_date_running = Window.orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_product = Window.partitionBy("product")
w_product_by_row_count = Window.partitionBy("product").orderBy(F.col("row_count").desc(), "holder_id", "product")
w_all_by_total_premium_moving = Window.orderBy("total_premium", "holder_id", "product").rowsBetween(-6, Window.currentRow)

valid_policys = (
    df_policies
    .filter((F.col("holder_id") >= 17) & (F.col("status").isin("ACTIVE", "EXPIRED")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
        F.concat_ws("-", F.col("product"), F.col("status")).alias("product_status_label"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("end_date")).alias("days_since_end_date"),
    )
)

sequenced = (
    valid_policys
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
        "product_status_label",
        "days_since_end_date",
        F.lag("days_since_end_date", 2).over(w_product_end_date_month_by_en).alias("prev2_days_since_end_date"),
        F.max("premium").over(w_all_by_end_date_running).alias("max_premium_so_far"),
        F.min("days_since_end_date").over(w_product).alias("product_min_days_since_end_date"),
    )
)

per_group = (
    sequenced
    .groupBy("holder_id", "product")
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("product").isin("HEALTH", "LIFE"), F.col("premium")).otherwise(0)).alias("premium_product_in_health_life"),
        F.sum("premium").alias("total_premium"),
        F.countDistinct("holder_id").alias("distinct_holder_ids"),
    )
    .filter(F.col("row_count") >= 2)
)

ranked = (
    per_group
    .select(
        "holder_id",
        "product",
        "row_count",
        "premium_product_in_health_life",
        "total_premium",
        F.lag("distinct_ids", 1, 0).over(w_product_by_row_count).alias("prev_distinct_ids"),
        F.round(F.avg("row_count").over(w_all_by_total_premium_moving), 2).alias("row_count_avg_7"),
    )
)

result = (
    ranked
    .select(
        "holder_id",
        "product",
        F.col("row_count_avg_7").alias("row_count_avg_7_v"),
        F.col("premium_product_in_health_life").alias("m_premium_product_in_health_life"),
        F.col("total_premium").alias("total_premium_v"),
    )
    .unpivot(
        ids=["holder_id", "product"],
        values=["row_count_avg_7_v", "m_premium_product_in_health_life", "total_premium_v"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
