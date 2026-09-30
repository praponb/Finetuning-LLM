from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.partitionBy("country").orderBy("rating", "id")
w3 = Window.partitionBy("city").orderBy("rating", "id")
w4 = Window.orderBy("rating", "id")

result = (
    df_suppliers
    .withColumn("country_sum_rating", F.sum("rating").over(w))
    .withColumn("next_rating", F.lead("rating", 2).over(w2))
    .withColumn("rating_bucket", F.ntile(5).over(w3))
    .withColumn("row_seq", F.row_number().over(w4))
    .withColumn("next_rating_2", F.lead("rating", 1).over(w4))
)
