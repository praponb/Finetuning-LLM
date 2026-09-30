from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("city") == "Singapore") & ~F.col("is_active"))
    .select("id", "country", F.initcap(F.trim("name")).alias("name_title"))
)
