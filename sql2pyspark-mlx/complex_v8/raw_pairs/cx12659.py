from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_distinct_ids_by_distinct_pro = Window.partitionBy("distinct_ids").orderBy("distinct_products")
w_distinct_ids_end_date_month = Window.partitionBy("distinct_ids", F.year("end_date_month"))
w_distinct_ids_by_distinct_pro_2 = Window.partitionBy("distinct_ids").orderBy("distinct_products", "end_date_month", "product")

recent = (
    df_policies
    .filter((F.col("start_date").isNotNull()) & (F.col("status").isin("EXPIRED", "CANCELLED", "ACTIVE")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
        F.concat(F.col("status"), F.lit("#"), F.col("id").cast("string")).alias("status_ref"),
    )
)

stats = (
    recent
    .groupBy(F.date_trunc("month", "end_date").alias("end_date_month"), "product")
    .agg(
        F.max("premium").alias("max_premium"),
        F.max("premium_filled").alias("max_premium_filled"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("product").alias("distinct_products"),
    )
)

ranked = (
    stats
    .select(
        "*",
        F.dense_rank().over(w_distinct_ids_by_distinct_pro).alias("distinct_products_dense_rank"),
        F.size(F.collect_set("product").over(w_distinct_ids_end_date_month)).alias("distinct_product_in_distinct_ids_end_date_month_"),
        F.row_number().over(w_distinct_ids_by_distinct_pro_2).alias("seq_no"),
        F.round(F.col("max_premium_filled") / F.nullif(F.sum("max_premium_filled").over(w_distinct_ids_end_date_month), F.lit(0)), 4).alias("max_premium_filled_share_of_distinct_ids_end_dat"),
        F.ntile(4).over(w_distinct_ids_by_distinct_pro_2).alias("distinct_products_quartile"),
    )
)

result = (
    ranked
    .select(
        "end_date_month",
        "product",
        "distinct_product_in_distinct_ids_end_date_month_",
        "seq_no",
    )
    .unpivot(
        ids=["end_date_month", "product"],
        values=["distinct_product_in_distinct_ids_end_date_month_", "seq_no"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("end_date_month", "product", "metric_name")
)
