from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("shipped_date", "id")
w2 = Window.partitionBy("carrier")

result = (
    df_shipments
    .select(
        "id",
        "carrier",
        "shipped_date",
        "cost",
        (F.col("cost") - F.lag("cost", 1).over(w)).alias("cost_change"),
        F.round((F.col("cost") / F.sum("cost").over(w2)), 3).alias("cost_share"),
    )
)
