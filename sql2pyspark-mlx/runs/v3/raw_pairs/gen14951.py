from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("region") == "North") & (~F.col("region").isin(["West", "North"]) | F.month("opened_date").isin([1, 2, 5])))
    .withColumn("store_name_upper", F.upper("store_name"))
    .withColumn("sqft_filled", F.coalesce("sqft", F.lit(0)))
)
