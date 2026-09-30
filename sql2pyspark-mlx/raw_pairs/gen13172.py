from pyspark.sql import functions as F

result = (
    df_customers
    .filter((~F.col("country").isin(["US", "JP", "TH"]) & F.col("city").isin(["Tokyo", "New York"])) | F.col("name").like("Pi%"))
    .select("id", "email", F.concat("city", F.lit("/"), "name").alias("city_name"))
    .orderBy(F.col("email").desc())
)
