from pyspark.sql import functions as F

result = (
    df_customers
    .select("name", F.initcap(F.trim("country")).alias("country_title"))
    .orderBy("name")
)
