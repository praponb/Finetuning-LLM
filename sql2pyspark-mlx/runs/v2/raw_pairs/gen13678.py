from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("segment").isin(["SMB", "Corporate", "Consumer"]))
    .select("id", "country", F.concat("city", F.lit(" "), "email").alias("city_email"))
)
