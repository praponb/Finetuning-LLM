from pyspark.sql import functions as F

result = (
    df_stores
    .select(
        "region",
        "store_name",
        "opened_date",
        F.round((F.col("sqft") * 0.9), 2).alias("sqft_scaled"),
    )
)
