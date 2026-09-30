from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") > 4) & F.col("unit_price").between(240, 320))
    .select("id", "product_id")
    .orderBy(F.col("id").desc())
)
