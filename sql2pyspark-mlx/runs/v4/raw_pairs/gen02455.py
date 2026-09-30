from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier")

result = (
    df_shipments
    .filter(F.col("cost") > 80)
    .select(
        "id",
        "carrier",
        "cost",
        F.round((F.col("cost") / F.sum("cost").over(w)), 3).alias("cost_share"),
    )
    .orderBy(F.col("cost").desc(), F.col("cost_share").desc())
)
