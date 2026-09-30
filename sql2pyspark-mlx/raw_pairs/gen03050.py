from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("shipped_date", "id")

result = (
    df_shipments
    .filter(F.col("cost") <= 94)
    .select(
        "id",
        "order_id",
        "shipped_date",
        "cost",
        F.lead("cost", 2).over(w).alias("next_cost"),
    )
    .orderBy("cost")
)
