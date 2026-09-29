from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("shipped_date").desc())

result = (
    df_shipments
    .filter(F.col("delivered_date").between("2024-03-01", "2024-06-01"))
    .select(
        "id",
        "status",
        "shipped_date",
        "order_id",
        F.dense_rank().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 2)
)
