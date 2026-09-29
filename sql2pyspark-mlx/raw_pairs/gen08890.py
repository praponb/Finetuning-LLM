from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("shipped_date", "id")

result = (
    df_shipments
    .select(
        "id",
        "carrier",
        "shipped_date",
        "status",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 3)
    .select("id", "carrier", "shipped_date", "status")
)
