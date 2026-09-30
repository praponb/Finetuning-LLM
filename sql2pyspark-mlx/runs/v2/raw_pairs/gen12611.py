from pyspark.sql import functions as F

result = (
    df_stores
    .withColumn("sqft_num", F.col("sqft").cast("string"))
    .withColumn("city_upper", F.upper("city"))
    .orderBy("opened_date")
)
