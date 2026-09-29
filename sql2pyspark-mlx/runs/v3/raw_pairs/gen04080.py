from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("category") != "Sports") & F.col("cost").between(180, 560) & F.col("supplier_id").isNotNull())
    .select("category", "tags")
)
