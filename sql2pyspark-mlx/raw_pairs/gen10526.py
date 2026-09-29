from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_date", "id")

result = (
    df_stores
    .select("id", "opened_date", "sqft", F.lag("sqft", 1).over(w).alias("prev_sqft"))
)
