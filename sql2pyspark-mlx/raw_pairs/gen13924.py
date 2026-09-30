from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("shipped_date").desc(), "id")

result = (
    df_shipments
    .filter(F.col("cost") < 48)
    .select(
        "id",
        "carrier",
        "shipped_date",
        "order_id",
        F.row_number().over(w).alias("rn"),
    )
    .filter(F.col("rn") <= 3)
    .select("id", "carrier", "shipped_date", "order_id")
)
