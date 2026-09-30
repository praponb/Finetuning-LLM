from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost").between(31, 35))
    .select("shipped_date")
    .unionByName(
        df_shipments
        .filter(F.year("delivered_date") == 2024)
        .select("shipped_date")
    )
    .distinct()
)
