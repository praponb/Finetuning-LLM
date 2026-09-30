from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("cost_rounded", "id")

filtered = (
    df_shipments
    .filter((F.col("cost") >= 31) & (F.col("delivered_date") > F.lit("2023-09-20").cast("date")))
    .withColumn("cost_rounded", F.round("cost", 2))
)

result = (
    filtered
    .filter(F.col("delivered_date").between("2024-01-07", "2025-08-01"))
    .select(
        "id",
        "order_id",
        "cost",
        "status",
        F.lead("cost").over(w).alias("next_cost"),
        F.lead("cost_rounded").over(w).alias("next_cost_rounded"),
        F.row_number().over(w).alias("row_seq"),
    )
)
