from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("delivered_date").desc(), "id")

result = (
    df_shipments
    .filter(F.col("status").isin(["RETURNED", "DELIVERED", "IN_TRANSIT"]))
    .select(
        "id",
        "carrier",
        "delivered_date",
        "cost",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 3)
    .orderBy("carrier", F.col("id").desc())
)
