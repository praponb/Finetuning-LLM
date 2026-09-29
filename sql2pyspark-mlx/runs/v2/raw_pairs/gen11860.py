from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("cost").between(30, 91) & F.col("delivered_date").between("2023-06-01", "2024-08-08")) | (F.col("cost") >= 85))
    .select(
        "cost",
        "status",
        "delivered_date",
        F.date_format("shipped_date", "MMM yyyy").alias("shipped_date_label"),
        F.date_format("shipped_date", "yyyy-MM-dd").alias("shipped_date_label_2"),
    )
    .orderBy("shipped_date_label_2", F.col("cost").desc())
)
