from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("delivered_date", "id")

result = (
    df_shipments
    .select(
        "id",
        "delivered_date",
        "cost",
        F.lead("cost", 1).over(w).alias("next_cost"),
    )
)
