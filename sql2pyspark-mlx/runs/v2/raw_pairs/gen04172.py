from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier", "status").orderBy(F.col("shipped_date").desc(), "id")

result = (
    df_shipments
    .filter(F.col("cost") <= 88)
    .select(
        "id",
        "carrier",
        "status",
        "shipped_date",
        F.row_number().over(w).alias("row_num"),
    )
    .orderBy(F.col("shipped_date").desc())
)
