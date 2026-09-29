from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("price").desc(), "id")
w2 = Window.partitionBy("supplier_id").orderBy(F.col("launch_date").desc_nulls_last(), "id")
w3 = Window.partitionBy("supplier_id").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("supplier_id").orderBy("stock_qty", "id")

not_flagged = (
    df_products
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.last_restock") < F.col("s.launch_date")), "left_semi")
)

latest = (
    not_flagged
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

latest_products = (
    latest
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

enriched = (
    latest_products
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "tags",
        "supplier_id",
        F.max("stock_qty").over(w3).alias("running_max_stock_qty"),
        F.min("stock_qty").over(w3).alias("running_min_stock_qty"),
        F.sum("stock_qty").over(w3).alias("running_sum_stock_qty"),
        F.lead("stock_qty", 1).over(w4).alias("next_stock_qty"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("running_sum_stock_qty") >= 1850, "LARGE").otherwise("SMALL").alias("running_sum_stock_qty_tier"),
    )
    .agg(
        F.size(F.collect_set("category")).alias("num_categories"),
        F.sum(F.when(F.col("brand") == "Umbrella", F.col("running_sum_stock_qty")).otherwise(0)).alias("umbrella_running_sum_stock_qty"),
        F.avg(F.when(F.col("brand") == "Acme", F.col("running_sum_stock_qty"))).alias("acme_avg_running_sum_stock_qty"),
        F.sum(F.when(F.col("brand") == "Stark", F.col("running_max_stock_qty")).otherwise(0)).alias("stark_running_max_stock_qty"),
        F.count(F.when(F.col("brand") == "Umbrella", 1)).alias("umbrella_count"),
        F.count(F.when(F.col("brand") == "Initech", 1)).alias("initech_count"),
    )
    .filter(F.col("stark_running_max_stock_qty") < 1000000)
)
