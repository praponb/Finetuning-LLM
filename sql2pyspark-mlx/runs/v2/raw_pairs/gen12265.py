from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("delivered_date").desc(), "id")

result = (
    df_shipments
    .filter(F.month("delivered_date").isin([8, 9, 12]))
    .select(
        "id",
        "status",
        "delivered_date",
        F.row_number().over(w).alias("delivered_date_row"),
    )
)
