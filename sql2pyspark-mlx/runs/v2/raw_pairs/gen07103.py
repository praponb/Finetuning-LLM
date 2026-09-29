from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").like("%Central%") & F.col("sqft").isNotNull())
    .groupBy("region", F.quarter("opened_date").alias("opened_quarter"))
    .agg(
        F.count("*").alias("num_stores"),
        F.max("opened_date").alias("latest_opened_date"),
    )
    .filter(F.col("num_stores") > 4)
)
