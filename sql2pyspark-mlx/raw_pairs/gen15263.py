from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id", "brand").orderBy(F.col("price").desc(), "id")

ranked = (
    df_products
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    ranked
    .groupBy(
        F.when(F.col("cost") >= 730, "PREMIUM").otherwise("STANDARD").alias("cost_tier"),
    )
    .agg(
        F.count(F.when(F.col("category") == "Home", 1)).alias("home_rows"),
        F.size(F.collect_set("brand")).alias("num_brands"),
        F.corr("price", "stock_qty").alias("corr_price_stock_qty"),
        F.countDistinct("supplier_id").alias("distinct_supplier_ids"),
    )
    .filter(F.col("num_brands") < 1000)
    .orderBy(F.col("num_brands").desc_nulls_last())
)
