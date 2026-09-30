from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("is_active") & (F.length("name") > 13)) | (F.lower(F.trim("name")) == "olivia garcia"))
    .groupBy(F.year("signup_date").alias("signup_year"))
    .agg(F.count("*").alias("row_count"))
    .orderBy(F.col("row_count").desc(), F.col("signup_year").desc())
)
