from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date")

result = (
    df_stores
    .select("id", "city", "opened_date", F.rank().over(w).alias("opened_date_rank"))
)
