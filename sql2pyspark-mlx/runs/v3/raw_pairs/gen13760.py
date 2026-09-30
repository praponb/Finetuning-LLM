from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("shipped_date").desc(), "id")

result = (
    df_shipments
    .filter(F.col("cost").between(47, 62))
    .select(
        "id",
        "status",
        "shipped_date",
        "order_id",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 3)
    .select("id", "status", "shipped_date", "order_id")
)
