from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.length("store_name") < 12)
    .select(
        "id",
        "store_name",
        "opened_date",
        F.round((F.col("sqft") * 1.07), 2).alias("adjusted_sqft"),
    )
)
