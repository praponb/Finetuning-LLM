from pyspark.sql import functions as F

result = (
    df_products
    .select("supplier_id", "category", F.initcap(F.trim("brand")).alias("brand_title"))
)
