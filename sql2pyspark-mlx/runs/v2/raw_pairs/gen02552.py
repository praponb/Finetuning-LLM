from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("shipped_date", "id")

result = (
    df_shipments
    .select(
        "id",
        "shipped_date",
        "cost",
        (F.col("cost") - F.lag("cost", 2, 0).over(w)).alias("cost_change"),
    )
)
