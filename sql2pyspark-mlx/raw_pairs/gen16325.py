from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("brand", "category").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("supplier_id").orderBy("product_name_length", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.orderBy("cost", "id").rowsBetween(-6, Window.currentRow)

valid = (
    df_products
    .filter(F.col("stock_qty") >= 126)
    .withColumn("category_product_name_key", F.concat_ws(",", "category", "product_name"))
    .withColumn("product_name_length", F.length("product_name"))
)

enriched = (
    valid
    .select(
        "id",
        "product_name",
        "price",
        "cost",
        "tags",
        "supplier_id",
        "category_product_name_key",
        "product_name_length",
        F.sum("product_name_length").over(w_recent).alias("running_sum_product_name_length"),
        F.count("product_name_length").over(w_recent).alias("running_count_product_name_length"),
        F.avg("product_name_length").over(w_recent).alias("running_avg_product_name_length"),
    )
)

scored = (
    enriched
    .select(
        "id",
        "product_name",
        "cost",
        "category_product_name_key",
        "product_name_length",
        "running_sum_product_name_length",
        "running_count_product_name_length",
        "running_avg_product_name_length",
        F.avg("cost").over(w2).alias("last4_avg_cost"),
        F.max("cost").over(w2).alias("last4_max_cost"),
        F.min("cost").over(w2).alias("last4_min_cost"),
    )
)

metrics = (
    scored
    .select(
        "id",
        "cost",
        "category_product_name_key",
        "running_sum_product_name_length",
        "last4_avg_cost",
        "last4_max_cost",
        F.max("product_name_length").over(w3).alias("last7_max_product_name_length"),
        F.avg("product_name_length").over(w3).alias("last7_avg_product_name_length"),
        F.min("product_name_length").over(w3).alias("last7_min_product_name_length"),
        F.sum("product_name_length").over(w3).alias("last7_sum_product_name_length"),
    )
)

daily = (
    metrics
    .rollup(
        F.when(F.col("last4_max_cost") >= 720, "LARGE").otherwise("SMALL").alias("last4_max_cost_tier"),
        F.when(F.col("last4_avg_cost") >= 680, "PREMIUM").otherwise("STANDARD").alias("last4_avg_cost_tier"),
    )
    .agg(
        F.percentile_approx("running_sum_product_name_length", 0.9).alias("running_sum_product_name_length_p90"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("num_rows"),
    )
)

result = (
    daily
    .select(
        "last4_max_cost_tier",
        "last4_avg_cost_tier",
        "running_sum_product_name_length_p90",
        "distinct_ids",
        "num_rows",
    )
    .orderBy(F.col("last4_max_cost_tier").asc_nulls_last())
)
