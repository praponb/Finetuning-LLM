from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("units").desc(), "id")

result = (
    df_store_sales
    .select("id", "store_id", "units", F.ntile(3).over(w).alias("quartile"))
)
