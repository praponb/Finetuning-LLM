from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("country") == "TH") & (F.col("city") != "Berlin"))
    .withColumn("rating_whole", F.floor("rating"))
    .withColumn("city_upper", F.upper("city"))
)
