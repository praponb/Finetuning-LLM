from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date").between("2025-07-01", "2025-10-11"))
    .select(
        "id",
        "cost",
        "shipped_date",
        "order_id",
        F.round((F.col("cost") * 0.9), 2).alias("adjusted_cost"),
    )
    .orderBy(F.col("order_id").desc(), F.col("id").desc())
)
