from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date", "id")
w2 = Window.orderBy("opened_date", "id")

result = (
    df_stores
    .select(
        "id",
        "city",
        "opened_date",
        "sqft",
        F.ntile(10).over(w).alias("quartile"),
        F.first("sqft").over(w2).alias("first_sqft"),
    )
    .orderBy("sqft")
)
