from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("category") == "Sports") & (F.size("tags") > 1))
    .select("launch_date", "tags")
    .orderBy(F.col("launch_date").desc())
)
