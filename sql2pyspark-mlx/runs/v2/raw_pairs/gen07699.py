from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost").between(76, 95) & (F.col("status") != "RETURNED"))
    .select(
        "id",
        "shipped_date",
        F.round((F.col("cost") * 100), 2).alias("adjusted_cost"),
        F.round((F.col("cost") * 100), 2).alias("adjusted_cost_2"),
    )
    .orderBy("shipped_date", "id")
)
