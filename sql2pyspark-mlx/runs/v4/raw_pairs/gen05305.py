from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("shipped_date", "id")

result = (
    df_shipments
    .filter(F.col("carrier").isNotNull())
    .select(
        "id",
        "order_id",
        "shipped_date",
        "cost",
        F.row_number().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") == 1)
    .orderBy("id")
)
