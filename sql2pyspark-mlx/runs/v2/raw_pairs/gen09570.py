from pyspark.sql import functions as F

result = (
    df_shipments
    .cube(
        F.year("shipped_date").alias("shipped_year"),
        F.year("delivered_date").alias("delivered_year"),
    )
    .agg(F.min("shipped_date").alias("first_shipped_date"))
)
