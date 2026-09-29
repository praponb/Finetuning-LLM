from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())

valid = (
    df_products
    .filter((F.col("launch_date") >= F.to_date(F.lit("2023-06-08"))) & (F.lower(F.trim("product_name")) == "wireless mouse"))
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.concat_ws("-", "brand", "product_name").alias("brand_product_name_key"),
        F.floor("stock_qty").alias("stock_qty_whole"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("resolved_brand"),
    )
)

result = (
    valid
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.sale_date"), F.col("c.revenue"))
)
