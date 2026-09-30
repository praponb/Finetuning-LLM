from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        "id",
        "delivered_date",
        F.date_add("delivered_date", 43).alias("delivered_date_plus_43d"),
    )
    .orderBy(F.col("delivered_date_plus_43d").desc())
)
