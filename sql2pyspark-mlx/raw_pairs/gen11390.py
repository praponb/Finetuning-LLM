from pyspark.sql import functions as F

result = (
    df_stores
    .cube(
        F.month("opened_date").alias("opened_month"),
        F.when(F.col("city").isin(["New York", "Bangkok"]), "core").otherwise("other").alias("city_group"),
    )
    .agg((F.max("sqft") - F.min("sqft")).alias("sqft_range"))
)
