from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("shipped_date", "id")

result = (
    df_shipments
    .select("id", "shipped_date", "cost", F.sum("cost").over(w).alias("running_cost"))
    .orderBy(F.col("cost").desc(), F.col("id").desc())
)
