from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("opened_date").between("2024-07-01", "2024-08-15") & (F.col("sqft") <= 12870)) | F.col("city").isin(["Tokyo", "Berlin", "New York"]))
    .select("id", "store_name", "city", F.col("sqft").cast("double").alias("sqft_num"))
    .orderBy("sqft_num")
)
