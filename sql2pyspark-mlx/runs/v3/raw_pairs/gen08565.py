from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("shipped_date")

result = (
    df_shipments
    .filter(F.col("shipped_date").between("2023-04-27", "2025-05-01"))
    .select(
        "id",
        "carrier",
        "shipped_date",
        F.rank().over(w).alias("shipped_date_rank"),
    )
)
