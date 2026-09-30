from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("cost").desc(), "id")

result = (
    df_shipments
    .filter(F.col("shipped_date") <= "2024-09-01")
    .select(
        "id",
        "carrier",
        "cost",
        "shipped_date",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 1)
    .orderBy("id")
)
