from pyspark.sql import functions as F

result = df_customers.withColumn("country_email", F.concat("country", F.lit(" "), "email"))
