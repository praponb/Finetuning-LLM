from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("price", "id")

latest = (
    df_products
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "brand", "cost", "stock_qty", "launch_date", "tags", "supplier_id")
)

result = (
    latest
    .groupBy(
        F.when(F.col("cost") >= 690, "PREMIUM").otherwise("STANDARD").alias("cost_tier"),
        "supplier_id",
    )
    .agg(
        F.round(F.sum("cost") / F.nullif(F.count(F.when(F.col("brand") == "Acme", 1)), F.lit(0)), 2).alias("cost_per_acme"),
        F.stddev_samp("stock_qty").alias("stock_qty_stddev"),
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
    )
    .orderBy(F.col("stock_qty_stddev").desc())
)
