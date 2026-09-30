from pyspark.sql import functions as F

result = (
    df_stores
    .withColumn("city_upper", F.upper("city"))
    .orderBy("store_name", "id")
    .limit(50)
)
