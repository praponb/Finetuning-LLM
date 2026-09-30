from pyspark.sql import functions as F

result = (
    df_stores
    .select(
        "id",
        "sqft",
        "region",
        "opened_date",
        F.round((F.col("sqft") * 1.1), 2).alias("sqft_scaled"),
        F.date_trunc("year", "opened_date").alias("opened_date_period"),
    )
)
