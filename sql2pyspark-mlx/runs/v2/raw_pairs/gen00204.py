from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.year("opened_date").alias("opened_year"),
        F.quarter("opened_date").alias("opened_quarter"),
    )
    .agg((F.max("sqft") - F.min("sqft")).alias("sqft_range"))
    .filter(F.col("sqft_range") < 65)
)
