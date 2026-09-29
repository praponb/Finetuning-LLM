from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")

result = (
    df_shipments
    .filter(F.col("delivered_date") < "2023-09-14")
    .select(
        "id",
        "status",
        "cost",
        F.round((F.col("cost") / F.sum("cost").over(w)), 3).alias("cost_share"),
    )
    .orderBy("status", "cost_share")
)
