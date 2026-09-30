from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.orderBy("revenue", "id")

with_windows = (
    df_store_sales
    .withColumn("corr_revenue_units", F.corr("revenue", "units").over(w))
    .withColumn("revenue_bucket", F.ntile(4).over(w2))
)

result = (
    with_windows
    .select("id", "store_id")
    .orderBy("id")
)
