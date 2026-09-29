from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("cost").desc(), "id")

result = (
    df_shipments
    .filter(F.col("shipped_date").between("2023-04-05", "2025-01-01"))
    .select(
        "id",
        "order_id",
        "cost",
        "delivered_date",
        F.row_number().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") <= 2)
    .select("id", "order_id", "cost", "delivered_date")
    .orderBy(F.col("cost").desc(), F.col("delivered_date").desc())
)
