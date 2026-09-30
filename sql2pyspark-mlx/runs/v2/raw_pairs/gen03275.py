from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.upper(F.trim("product_name")) == "YOGA MAT")
    .select("id", "supplier_id", F.coalesce("price", F.lit(0)).alias("price_filled"))
    .orderBy("id")
)
