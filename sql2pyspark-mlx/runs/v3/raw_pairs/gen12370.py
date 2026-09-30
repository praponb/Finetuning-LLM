from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("shipped_date").desc(), "id")

result = (
    df_shipments
    .select(
        "id",
        "status",
        "shipped_date",
        "carrier",
        F.row_number().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 3)
    .orderBy("shipped_date")
)
