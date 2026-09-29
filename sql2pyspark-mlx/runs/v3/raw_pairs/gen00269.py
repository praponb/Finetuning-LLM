from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("shipped_date").desc(), "id")

result = (
    df_shipments
    .select(
        "id",
        "order_id",
        "shipped_date",
        "status",
        F.row_number().over(w).alias("pos"),
    )
    .filter(F.col("pos") == 1)
    .select("id", "order_id", "shipped_date", "status")
    .orderBy("shipped_date")
)
