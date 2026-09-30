from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_date", "id")

result = (
    df_stores
    .filter(F.col("city") == "Singapore")
    .select("id", "opened_date", F.ntile(4).over(w).alias("quartile"))
)
