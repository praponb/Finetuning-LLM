from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city") == "London")
    .select("name", F.initcap(F.trim("city")).alias("city_title"))
    .orderBy("city_title")
)
