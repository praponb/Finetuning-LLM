from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city")
w_grp = Window.orderBy("rating", "id")

metrics = (
    df_suppliers
    .withColumn("rating_share", F.round((F.col("rating") / F.sum("rating").over(w)), 4))
    .withColumn("next_rating", F.lead("rating", 1).over(w_grp))
)

result = (
    metrics
    .select("id", "supplier_name", "country", "city", "rating", "next_rating")
    .orderBy("id")
)
